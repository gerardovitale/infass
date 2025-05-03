import '@testing-library/jest-dom';

import React from 'react';
import { render, screen } from '@testing-library/react';
import { Navbar } from './Navbar';

jest.mock('./SearchProduct', () => ({
    SearchProduct: () => (
        <div data-testid="search-product-mock">SearchProduct Mock</div>
    ),
}));

describe('Navbar Component', () => {
    it('should render the Navbar component', () => {
        render(<Navbar />);
        const navbarElement = screen.getByRole('navigation');
        expect(navbarElement).toBeInTheDocument();
    });

    it('should render the SearchProduct component', () => {
        render(<Navbar />);
        const searchProductMock = screen.getByTestId('search-product-mock');
        expect(searchProductMock).toBeInTheDocument();
    });
});
