import '@testing-library/jest-dom';
import { render } from '@testing-library/react';
import SearchResult from './SearchResult';

// Mock the useSearchParams hook
jest.mock('next/navigation', () => ({
    useSearchParams: jest.fn(() => ({
        get: jest.fn(() => 'test product'),
    })),
}));

describe('SearchResult', () => {
    it('should display the search result', () => {
        const { getByText } = render(<SearchResult />);

        // Check if the search value is displayed
        expect(getByText('You searched: test product')).toBeInTheDocument();
    });
});
