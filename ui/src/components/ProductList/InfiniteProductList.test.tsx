import '@testing-library/jest-dom';
import { render, screen, act, waitFor } from '@testing-library/react';
import InfiniteProductList from './InfiniteProductList';
import { Product } from '@/types';

const makeProduct = (id: string): Product => ({
    id,
    name: `Product ${id}`,
    size: '100ml',
    current_price: 1.0,
    categories: 'Cat',
});

let intersectionCallback: (entries: { isIntersecting: boolean }[]) => void;

beforeEach(() => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (global as any).IntersectionObserver = jest.fn((cb: (entries: { isIntersecting: boolean }[]) => void) => {
        intersectionCallback = cb;
        return {
            observe: jest.fn(),
            disconnect: jest.fn(),
        };
    });
    (global.fetch as jest.Mock) = jest.fn();
});

afterEach(() => {
    jest.restoreAllMocks();
});

describe('InfiniteProductList', () => {
    it('renders initial products', () => {
        const products = [makeProduct('1'), makeProduct('2')];
        const { getAllByRole } = render(
            <InfiniteProductList
                initialProducts={products}
                initialHasMore={false}
                searchTerm="test"
                limit={20}
            />
        );
        expect(getAllByRole('listitem')).toHaveLength(2);
    });

    it('loads more products when sentinel is intersecting', async () => {
        const page2 = [makeProduct('3'), makeProduct('4')];
        (global.fetch as jest.Mock).mockResolvedValueOnce({
            json: () => Promise.resolve({ results: page2, has_more: false }),
        });

        const { getAllByRole } = render(
            <InfiniteProductList
                initialProducts={[makeProduct('1'), makeProduct('2')]}
                initialHasMore={true}
                searchTerm="test"
                limit={2}
            />
        );

        await act(async () => {
            intersectionCallback([{ isIntersecting: true }]);
        });

        await waitFor(() => {
            expect(getAllByRole('listitem')).toHaveLength(4);
        });

        expect(global.fetch).toHaveBeenCalledWith(
            '/api/search?search_term=test&limit=2&offset=2'
        );
    });

    it('does not render spinner when has_more is false', () => {
        render(
            <InfiniteProductList
                initialProducts={[makeProduct('1')]}
                initialHasMore={false}
                searchTerm="test"
                limit={20}
            />
        );
        expect(screen.queryByRole('status', { name: 'Loading' })).not.toBeInTheDocument();
    });

    it('shows spinner while loading more products and hides it after', async () => {
        let resolveResponse: (value: unknown) => void;
        const responsePromise = new Promise((resolve) => {
            resolveResponse = resolve;
        });

        (global.fetch as jest.Mock).mockReturnValueOnce({
            json: () => responsePromise,
        });

        render(
            <InfiniteProductList
                initialProducts={[makeProduct('1')]}
                initialHasMore={true}
                searchTerm="test"
                limit={1}
            />
        );

        await act(async () => {
            intersectionCallback([{ isIntersecting: true }]);
        });

        expect(screen.getByRole('status', { name: 'Loading' })).toBeInTheDocument();

        await act(async () => {
            resolveResponse!({ results: [makeProduct('2')], has_more: false });
        });

        await waitFor(() => {
            expect(screen.queryByRole('status', { name: 'Loading' })).not.toBeInTheDocument();
        });
    });
});
