import '@testing-library/jest-dom';
import { render, screen } from '@testing-library/react';
import { Suspense } from 'react';

describe('SearchPage Suspense behavior', () => {
    it('renders spinner fallback while search results are loading', () => {
        const SlowChild = () => {
            throw new Promise(() => {});
        };

        render(
            <Suspense
                fallback={
                    <div role="status" aria-label="Loading">
                        loading
                    </div>
                }
            >
                <SlowChild />
            </Suspense>
        );

        const spinner = screen.getByRole('status', { name: 'Loading' });
        expect(spinner).toBeInTheDocument();
    });

    it('hides spinner once content resolves', () => {
        render(
            <Suspense
                fallback={
                    <div role="status" aria-label="Loading">
                        loading
                    </div>
                }
            >
                <div>Search results loaded</div>
            </Suspense>
        );

        expect(screen.queryByRole('status', { name: 'Loading' })).not.toBeInTheDocument();
        expect(screen.getByText('Search results loaded')).toBeInTheDocument();
    });
});
