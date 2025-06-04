import '@testing-library/jest-dom';
import { render } from '@testing-library/react';
import SearchResult from './SearchResult';
import { ProductResponse } from '@/types';

const mockedProducts = {
    results: [
        { id: '1', name: 'Milk', size: '1L', price: 2.5, category: 'Drinks' },
        {
            id: '2',
            name: 'Almond Milk',
            size: '1L',
            price: 3.0,
            category: 'Drinks',
        },
    ],
    total_results: 2,
};
const fetchMock = jest.fn(() =>
    Promise.resolve({
        ok: true,
        json: () => Promise.resolve<ProductResponse>(mockedProducts),
    })
);
global.fetch = fetchMock as unknown as typeof fetch;

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
            ok: true,
            json: () => Promise.resolve({ results: [], total_results: 0 }),
        });
        const { queryByText } = await renderSearchPage();
        const noResults = queryByText('No products found');
        expect(noResults).toBeInTheDocument();

    });
});
