import '@testing-library/jest-dom';
import { render } from '@testing-library/react';
import SearchResult from './SearchResult';

jest.mock('next/cache', () => ({
    unstable_cache: (fn: Function) => fn,
}));

jest.mock('../ProductList/InfiniteProductList', () => {
    return function MockInfiniteProductList(props: { initialProducts: { id: string }[] }) {
        return (
            <ul>
                {props.initialProducts.map((p) => (
                    <li key={p.id}>{p.id}</li>
                ))}
            </ul>
        );
    };
});

const mockedProducts = {
    results: [
        {
            id: '1',
            name: 'Milk',
            size: '1L',
            current_price: 2.5,
            categories: 'Drinks',
        },
        {
            id: '2',
            name: 'Almond Milk',
            size: '1L',
            current_price: 3.0,
            categories: 'Drinks',
        },
    ],
    total_results: 2,
    limit: 20,
    offset: 0,
    has_more: false,
};
const fetchMock = jest.fn();

jest.mock('google-auth-library', () => {
    return {
        GoogleAuth: jest.fn().mockImplementation(() => {
            return {
                getIdTokenClient: jest.fn().mockResolvedValue({
                    fetch: fetchMock.mockResolvedValue({
                        data: mockedProducts,
                        status: 200,
                    }),
                }),
            };
        }),
    };
});
async function renderSearchPage() {
    const ui = await SearchResult({ productSearched: 'milk' });
    return render(<>{ui}</>);
}

describe('SearchResult', () => {
    it('should display the search result in a list', async () => {
        const { getByRole } = await renderSearchPage();
        const list = getByRole('list');

        expect(list.children).toHaveLength(2);
    });

    it('should render an empty list if no search results are found', async () => {
        fetchMock.mockResolvedValueOnce({
            status: 200,
            data: { results: [], total_results: 0, limit: 20, offset: 0, has_more: false },
        });
        const { queryByText } = await renderSearchPage();
        const noResults = queryByText('No products found');
        expect(noResults).toBeInTheDocument();
    });
});
