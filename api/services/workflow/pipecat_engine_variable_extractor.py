from __future__ import annotations

from typing import TYPE_CHECKING, Any, List

from loguru import logger
from opentelemetry import trace

from api.services.gen_ai.json_parser import parse_llm_json
from api.services.pipecat.tracing_config import is_tracing_enabled
from api.services.workflow.dto import ExtractionVariableDTO
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.utils.tracing.service_attributes import add_llm_span_attributes

if TYPE_CHECKING:
    from api.services.workflow.pipecat_engine import PipecatEngine


class VariableExtractionManager:
    """Helper that registers and executes the \"extract_variables\" tool.

    The manager is responsible for two things:
      1. Registering a callable with the LLM service so that the tool can be
         invoked from within the model.
      2. Executing the extraction in a background task while maintaining
         correct bookkeeping and optional OpenTelemetry tracing.
    """

    def __init__(self, engine: "PipecatEngine") -> None:  # noqa: F821
        # We keep a reference to the engine so we can reuse its context
        # and update internal counters / extracted variable state.
        self._engine = engine
        self._context = engine.context

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    async def _perform_extraction(
        self,
        extraction_variables: List[ExtractionVariableDTO],
        parent_ctx: Any,
        extraction_prompt: str = "",
    ) -> dict:
        """Run the actual extraction chat completion and post-process the result."""

        # ------------------------------------------------------------------
        # Build the prompt that instructs the model to extract the variables.
        # ------------------------------------------------------------------
        vars_description = "\n".join(
            f"- {v.name} ({v.type}): {v.prompt}" for v in extraction_variables
        )

        # ------------------------------------------------------------------
        # Build a normalized representation of the existing conversation so the
        # extractor works with both OpenAI-style (dict) messages and Google
        # Gemini `Content` objects.
        # ------------------------------------------------------------------
        def _get_role_and_content(msg: Any) -> tuple[str | None, str | None]:
            """Return a pair of (role, content) for the given message.

            The logic supports both OpenAI-style dict messages and Google
            `Content` objects that expose ``role`` and ``parts`` attributes.
            Only plain textual content is extracted – image parts, tool call
            placeholders, etc. are ignored for the purpose of variable
            extraction.
            """

            # --------------------------------------------------------------
            # OpenAI format → simple dict with ``role`` and ``content`` keys
            # --------------------------------------------------------------
            if isinstance(msg, dict):
                role = msg.get("role")
                content_field = msg.get("content")

                # Content can be a str, list of segments, or None.
                if isinstance(content_field, str):
                    content = content_field
                elif isinstance(content_field, list):
                    # Collapse all text parts into a single string.
                    texts = [
                        segment.get("text", "")
                        for segment in content_field
                        if isinstance(segment, dict) and segment.get("type") == "text"
                    ]
                    content = " ".join(texts) if texts else None
                else:
                    content = None

                return role, content

            # --------------------------------------------------------------
            # Google Gemini format → ``Content`` object with ``parts`` list
            # --------------------------------------------------------------
            role_attr = getattr(msg, "role", None)
            parts_attr = getattr(msg, "parts", None)

            if role_attr is None or parts_attr is None:
                return None, None  # Unrecognised message format

            role = (
                "assistant" if role_attr == "model" else role_attr
            )  # Normalise role name

            # Collect textual parts only (ignore images, function calls, etc.)
            texts: list[str] = []
            for part in parts_attr:
                text_val = getattr(part, "text", None)
                if text_val:
                    texts.append(text_val)

            content = " ".join(texts) if texts else None
            return role, content

        conversation_lines: list[str] = []
        for msg in self._context.messages:
            role, content = _get_role_and_content(msg)
            if role in ("assistant", "user") and content:
                conversation_lines.append(f"{role}: {content}")

        conversation_history = "\n".join(conversation_lines)

        system_prompt = (
            "You are an assistant tasked with extracting structured data from a conversation. "
            "Return ONLY a valid JSON object with the requested variables as top-level keys. Do not wrap the JSON in markdown."
        )
        # Use provided extraction_prompt as system prompt, or default
        system_prompt = (
            system_prompt + "\n\n" + extraction_prompt
            if extraction_prompt
            else system_prompt
        )

        user_prompt = (
            "\n\nVariables to extract:\n"
            f"{vars_description}"
            "\n\nConversation history:\n"
            f"{conversation_history}"
        )

        extraction_context = LLMContext()
        extraction_messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]
        extraction_context.set_messages(extraction_messages)

        # ------------------------------------------------------------------
        # Use engine's LLM for out-of-band inference (no pipeline frames)
        # ------------------------------------------------------------------
        llm_response = await self._engine.llm.run_inference(extraction_context)

        # Get model name for tracing
        model_name = getattr(self._engine.llm, "model_name", "unknown")

        if is_tracing_enabled():
            tracer = trace.get_tracer("pipecat")
            with tracer.start_as_current_span(
                "llm-variable-extraction", context=parent_ctx
            ) as span:
                add_llm_span_attributes(
                    span,
                    service_name=self._engine.llm.__class__.__name__,
                    model=model_name,
                    operation_name="llm-variable-extraction",
                    messages=extraction_messages,
                    output=llm_response,
                    stream=False,
                    parameters={},
                )

        # ------------------------------------------------------------------
        # Parse the assistant output – fall back to raw text if it is not valid JSON.
        # Uses parse_llm_json which handles common LLM mistakes like markdown
        # code blocks (```json ... ```) and extra text around the JSON.
        # ------------------------------------------------------------------
        if llm_response is None:
            logger.warning("Extractor returned no response; returning empty result.")
            extracted = {}
        else:
            extracted = parse_llm_json(llm_response)
            if "raw" in extracted and len(extracted) == 1:
                logger.warning(
                    "Extractor returned invalid JSON; storing raw content instead."
                )

        logger.debug(f"Extracted variables: {extracted}")
        return extracted

    async def _perform_call_tags_extraction(
        self,
        parent_ctx: Any,
        call_tags_prompt: str = "",
    ) -> list[str]:
        """Run a chat completion to extract call tags from the conversation.

        Returns a list of tag strings.
        """

        # ------------------------------------------------------------------
        # Build a normalized conversation history (reuses the same helper
        # logic from variable extraction).
        # ------------------------------------------------------------------
        def _get_role_and_content(msg: Any) -> tuple[str | None, str | None]:
            if isinstance(msg, dict):
                role = msg.get("role")
                content_field = msg.get("content")
                if isinstance(content_field, str):
                    content = content_field
                elif isinstance(content_field, list):
                    texts = [
                        segment.get("text", "")
                        for segment in content_field
                        if isinstance(segment, dict) and segment.get("type") == "text"
                    ]
                    content = " ".join(texts) if texts else None
                else:
                    content = None
                return role, content

            role_attr = getattr(msg, "role", None)
            parts_attr = getattr(msg, "parts", None)
            if role_attr is None or parts_attr is None:
                return None, None
            role = "assistant" if role_attr == "model" else role_attr
            texts: list[str] = []
            for part in parts_attr:
                text_val = getattr(part, "text", None)
                if text_val:
                    texts.append(text_val)
            content = " ".join(texts) if texts else None
            return role, content

        conversation_lines: list[str] = []
        for msg in self._context.messages:
            role, content = _get_role_and_content(msg)
            if role in ("assistant", "user") and content:
                conversation_lines.append(f"{role}: {content}")

        conversation_history = "\n".join(conversation_lines)

        system_prompt = (
            "You are an assistant tasked with extracting call tags from a conversation. "
            "Return ONLY a valid JSON array of short tag strings. Do not wrap the JSON in markdown. "
            "Example: [\"interested\", \"follow_up_needed\", \"pricing_discussed\"]"
        )
        if call_tags_prompt:
            system_prompt = system_prompt + "\n\n" + call_tags_prompt

        user_prompt = (
            "\n\nExtract relevant call tags from the following conversation:\n"
            f"{conversation_history}"
        )

        extraction_context = LLMContext()
        extraction_messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]
        extraction_context.set_messages(extraction_messages)

        llm_response = await self._engine.llm.run_inference(extraction_context)

        model_name = getattr(self._engine.llm, "model_name", "unknown")

        if is_tracing_enabled():
            tracer = trace.get_tracer("pipecat")
            with tracer.start_as_current_span(
                "llm-call-tags-extraction", context=parent_ctx
            ) as span:
                add_llm_span_attributes(
                    span,
                    service_name=self._engine.llm.__class__.__name__,
                    model=model_name,
                    operation_name="llm-call-tags-extraction",
                    messages=extraction_messages,
                    output=llm_response,
                    stream=False,
                    parameters={},
                )

        if llm_response is None:
            logger.warning("Call tags extractor returned no response; returning empty list.")
            return []

        parsed = parse_llm_json(llm_response)

        # parse_llm_json returns a dict. If the LLM returned a JSON array,
        # it will be stored under the "raw" key or similar.  We need to
        # handle both cases: a plain list from the LLM or a dict wrapper.
        if isinstance(parsed, list):
            tags = [str(t) for t in parsed if t]
        elif isinstance(parsed, dict):
            # If parse_llm_json wrapped a list in {"raw": ...}, try to
            # extract the list.  Otherwise flatten dict values.
            import json as _json
            raw = parsed.get("raw")
            if raw and isinstance(raw, str):
                try:
                    maybe_list = _json.loads(raw)
                    if isinstance(maybe_list, list):
                        tags = [str(t) for t in maybe_list if t]
                    else:
                        tags = []
                except _json.JSONDecodeError:
                    tags = []
            else:
                # Flatten any list values in the dict
                tags = []
                for v in parsed.values():
                    if isinstance(v, list):
                        tags.extend(str(t) for t in v if t)
                    elif isinstance(v, str) and v:
                        tags.append(v)
        else:
            tags = []

        logger.debug(f"Extracted call tags: {tags}")
        return tags
