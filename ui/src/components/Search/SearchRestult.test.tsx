import '@testing-library/jest-dom';
import { render } from '@testing-library/react';
import SearchResult from './SearchResult';

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
            data: { results: [], total_results: 0 },
        });
        const { queryByText } = await renderSearchPage();
        const noResults = queryByText('No products found');
        expect(noResults).toBeInTheDocument();
    });
});
