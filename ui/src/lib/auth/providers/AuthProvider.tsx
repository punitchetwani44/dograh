'use client';

import { Loader2 } from 'lucide-react';
import React, { createContext, lazy, Suspense, useContext } from 'react';

import type { AuthUser } from '../types';

// Shared context type for both Stack and Local providers
export interface AuthContextType {
  user: AuthUser | null;
  isAuthenticated: boolean;
  loading: boolean;
  getAccessToken: () => Promise<string>;
  redirectToLogin: () => void;
  logout: () => Promise<void>;
  provider: string;
  // Stack-specific (optional)
  getSelectedTeam?: () => unknown;
  listPermissions?: (team?: unknown) => Promise<Array<{ id: string }>>;
}

export const AuthContext = createContext<AuthContextType | null>(null);

// Lazy load provider wrappers only when needed
const StackProviderWrapper = lazy(() =>
  import('./StackProviderWrapper').then(module => ({
    default: module.StackProviderWrapper
  }))
);

const LocalProviderWrapper = lazy(() =>
  import('./LocalProviderWrapper').then(module => ({
    default: module.LocalProviderWrapper
  }))
);

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const authProvider = process.env.NEXT_PUBLIC_AUTH_PROVIDER || 'stack';

  // For Stack provider, use the dedicated wrapper
  if (authProvider === 'stack') {
    return (
      <Suspense fallback={
        <div className="flex items-center justify-center min-h-screen">
          <Loader2 className="w-8 h-8 animate-spin" />
        </div>
      }>
        <StackProviderWrapper>
          {children}
        </StackProviderWrapper>
      </Suspense>
    );
  }

  // For local/OSS provider
  return (
    <Suspense fallback={
      <div className="flex items-center justify-center min-h-screen">
        <Loader2 className="w-8 h-8 animate-spin" />
      </div>
    }>
      <LocalProviderWrapper>
        {children}
      </LocalProviderWrapper>
    </Suspense>
  );
}

export function useAuth() {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error('useAuth must be used within AuthProvider');
  }
  return context;
}
