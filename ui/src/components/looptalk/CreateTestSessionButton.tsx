'use client';

import { Plus } from 'lucide-react';
import { useRouter } from 'next/navigation';
import { useState } from 'react';
import { toast } from 'sonner';

import { createTestSessionApiV1LooptalkTestSessionsPost } from '@/client/sdk.gen';
import { Button } from '@/components/ui/button';
import {
    Dialog,
    DialogContent,
    DialogDescription,
    DialogFooter,
    DialogHeader,
    DialogTitle,
    DialogTrigger,
} from '@/components/ui/dialog';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import {
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue,
} from '@/components/ui/select';
import { Textarea } from '@/components/ui/textarea';
import { useAuth } from '@/lib/auth';
import logger from '@/lib/logger';

export function CreateTestSessionButton() {
    const router = useRouter();
    const [open, setOpen] = useState(false);
    const [loading, setLoading] = useState(false);
    const { user } = useAuth();
    const [formData, setFormData] = useState({
        name: '',
        description: '',
        test_type: 'single',
        actor_workflow_id: '',
        adversary_workflow_id: '',
        concurrent_pairs: 1,
    });

    const handleSubmit = async (e: React.FormEvent) => {
        e.preventDefault();
        setLoading(true);

        try {
            if (!user) return;
            const response = await createTestSessionApiV1LooptalkTestSessionsPost({
                body: {
                    name: formData.name,
                    actor_workflow_id: parseInt(formData.actor_workflow_id),
                    adversary_workflow_id: parseInt(formData.adversary_workflow_id),
                    config: {
                        test_type: formData.test_type,
                        description: formData.description,
                        concurrent_pairs: formData.test_type === 'load_test' ? formData.concurrent_pairs : undefined
                    }
                },
            });

            toast.success('Test session created successfully');
            setOpen(false);
            if (response.data?.id) {
                router.push(`/looptalk/${response.data.id}`);
            } else {
                router.push('/looptalk');
            }
        } catch (error) {
            logger.error('Error creating test session:', error);
            toast.error('Failed to create test session');
        } finally {
            setLoading(false);
        }
    };

    return (
        <Dialog open={open} onOpenChange={setOpen}>
            <DialogTrigger asChild>
                <Button>
                    <Plus className="h-4 w-4 mr-2" />
                    New Test Session
                </Button>
            </DialogTrigger>
            <DialogContent className="sm:max-w-[525px]">
                <form onSubmit={handleSubmit}>
                    <DialogHeader>
                        <DialogTitle>Create Test Session</DialogTitle>
                        <DialogDescription>
                            Set up a new LoopTalk test session to test conversations between agents.
                        </DialogDescription>
                    </DialogHeader>
                    <div className="grid gap-4 py-4">
                        <div className="grid gap-2">
                            <Label htmlFor="name">Test Name</Label>
                            <Input
                                id="name"
                                value={formData.name}
                                onChange={(e) => setFormData({ ...formData, name: e.target.value })}
                                placeholder="My Test Session"
                                required
                            />
                        </div>
                        <div className="grid gap-2">
                            <Label htmlFor="description">Description</Label>
                            <Textarea
                                id="description"
                                value={formData.description}
                                onChange={(e) => setFormData({ ...formData, description: e.target.value })}
                                placeholder="Optional description of the test"
                            />
                        </div>
                        <div className="grid gap-2">
                            <Label htmlFor="test_type">Test Type</Label>
                            <Select
                                value={formData.test_type}
                                onValueChange={(value) => setFormData({ ...formData, test_type: value })}
                            >
                                <SelectTrigger>
                                    <SelectValue />
                                </SelectTrigger>
                                <SelectContent>
                                    <SelectItem value="single">Single Test</SelectItem>
                                    {/* <SelectItem value="load_test">Load Test</SelectItem> */}
                                </SelectContent>
                            </Select>
                        </div>
                        <div className="grid gap-2">
                            <Label htmlFor="actor_workflow">Actor Workflow ID</Label>
                            <Input
                                id="actor_workflow"
                                type="number"
                                value={formData.actor_workflow_id}
                                onChange={(e) => setFormData({ ...formData, actor_workflow_id: e.target.value })}
                                placeholder="Enter workflow ID"
                                required
                            />
                        </div>
                        <div className="grid gap-2">
                            <Label htmlFor="adversary_workflow">Adversary Workflow ID</Label>
                            <Input
                                id="adversary_workflow"
                                type="number"
                                value={formData.adversary_workflow_id}
                                onChange={(e) => setFormData({ ...formData, adversary_workflow_id: e.target.value })}
                                placeholder="Enter workflow ID"
                                required
                            />
                        </div>
                        {formData.test_type === 'load_test' && (
                            <div className="grid gap-2">
                                <Label htmlFor="concurrent_pairs">Concurrent Pairs</Label>
                                <Input
                                    id="concurrent_pairs"
                                    type="number"
                                    min="1"
                                    max="10"
                                    value={formData.concurrent_pairs}
                                    onChange={(e) => setFormData({ ...formData, concurrent_pairs: parseInt(e.target.value) || 1 })}
                                    required
                                />
                            </div>
                        )}
                    </div>
                    <DialogFooter>
                        <Button type="submit" disabled={loading}>
                            {loading ? 'Creating...' : 'Create Test Session'}
                        </Button>
                    </DialogFooter>
                </form>
            </DialogContent>
        </Dialog>
    );
}
