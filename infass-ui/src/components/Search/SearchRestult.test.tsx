import '@testing-library/jest-dom';
import { render } from '@testing-library/react';
import SearchResult from './SearchResult';
import { decodeSearch } from './utils/searchUtils';

jest.mock('./utils/searchUtils');
const mockDecodeSearch = decodeSearch as jest.Mock;

describe('SearchResult', () => {
    it('should display the search result in a list', () => {
        mockDecodeSearch.mockReturnValue('product 1');
        const { getByRole } = render(<SearchResult />);
        const list = getByRole('list');

        // Check if the search value is displayed
        expect(list.children).toHaveLength(1);
    });

    it('should render an empty list if no search results are found', () => {
        mockDecodeSearch.mockReturnValue('nonexistent product');
        const { queryByRole } = render(<SearchResult />);
        const list = queryByRole('list');

        // Check if the list is not rendered
        expect(list).toBeEmptyDOMElement();
    });

    it('should handle multiple search results', () => {
        mockDecodeSearch.mockReturnValue('product');
        const { getByRole } = render(<SearchResult />);
        const list = getByRole('list');

        // Check if all search values are displayed
        expect(list.children).toHaveLength(3);
    });
});
