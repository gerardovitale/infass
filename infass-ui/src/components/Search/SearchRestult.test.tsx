import '@testing-library/jest-dom';
import { render } from '@testing-library/react';
import SearchResult from './SearchResult';
import { Product } from '@/types';

const mockedProducts = [
    { id: '1', name: 'Milk', size: '1L', price: 2.5, category: 'Drinks' },
    {
        id: '2',
        name: 'Almond Milk',
        size: '1L',
        price: 3.0,
        category: 'Drinks',
    },
];
const fetchMock = jest.fn(() =>
    Promise.resolve({
        ok: true,
        json: () => Promise.resolve<Product[]>(mockedProducts),
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
            json: () => Promise.resolve([]),
        });
        const { queryByRole } = await renderSearchPage();
        const list = queryByRole('list');

        // Check if the list is not rendered
        expect(list).toBeEmptyDOMElement();
    });
});
