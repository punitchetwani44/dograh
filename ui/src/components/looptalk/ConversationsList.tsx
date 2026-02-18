'use client';

import { format } from 'date-fns';
import { useEffect, useState } from 'react';

import { getTestSessionConversationApiV1LooptalkTestSessionsTestSessionIdConversationGet } from '@/client/sdk.gen';
import { Badge } from '@/components/ui/badge';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { useAuth } from '@/lib/auth';
import logger from '@/lib/logger';

import { Conversation } from './types';

interface ConversationsListProps {
    testSessionId: number;
}

export function ConversationsList({ testSessionId }: ConversationsListProps) {
    const [conversations, setConversations] = useState<Conversation[]>([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);
    const { user } = useAuth();

    useEffect(() => {
        const fetchConversations = async () => {
            if (!user) return;
            try {
                const response = await getTestSessionConversationApiV1LooptalkTestSessionsTestSessionIdConversationGet({
                    path: {
                        test_session_id: testSessionId
                    },
                });

                // API returns { conversation: Conversation | null }
                const responseData = response.data as { conversation: Conversation | null } | null;

                if (responseData?.conversation) {
                    setConversations([responseData.conversation]);
                } else {
                    setConversations([]);
                }
            } catch (err) {
                logger.error('Error fetching conversations:', err);
                setError('Failed to load conversations');
            } finally {
                setLoading(false);
            }
        };

        fetchConversations();

        // Poll for updates every 5 seconds
        const interval = setInterval(fetchConversations, 5000);
        return () => clearInterval(interval);
    }, [testSessionId, user]);

    if (loading && conversations.length === 0) {
        return (
            <div className="space-y-4">
                {Array.from({ length: 3 }, (_, i) => (
                    <Card key={i} className="h-24 bg-gray-200 animate-pulse" />
                ))}
            </div>
        );
    }

    if (error) {
        return (
            <div className="text-red-500">
                {error}
            </div>
        );
    }

    if (conversations.length === 0) {
        return (
            <Card>
                <CardContent className="text-center py-8">
                    <div className="text-gray-500 mb-2">
                        No conversations started yet
                    </div>
                    <p className="text-sm text-gray-400">
                        Start the test session to begin agent conversations
                    </p>
                </CardContent>
            </Card>
        );
    }

    const getStatusBadgeVariant = (status: string) => {
        switch (status) {
            case 'active':
                return 'default';
            case 'completed':
                return 'secondary';
            case 'failed':
                return 'destructive';
            default:
                return 'outline';
        }
    };

    return (
        <div className="space-y-4">
            {conversations.map((conversation) => (
                <Card key={conversation.id}>
                    <CardHeader>
                        <div className="flex justify-between items-start">
                            <div>
                                <CardTitle className="text-lg">
                                    Conversation {conversation.conversation_pair_id || conversation.id}
                                </CardTitle>
                                <CardDescription>
                                    Started: {format(new Date(conversation.created_at), 'h:mm:ss a')}
                                </CardDescription>
                            </div>
                            <Badge variant={getStatusBadgeVariant(conversation.status)}>
                                {conversation.status}
                            </Badge>
                        </div>
                    </CardHeader>
                </Card>
            ))}
        </div>
    );
}
