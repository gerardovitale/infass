import '@testing-library/jest-dom';
import { render, screen, fireEvent } from '@testing-library/react';
import { SearchProduct } from './SearchProduct';

describe('SearchProduct', () => {
    it('renders the input and search button', () => {
        render(<SearchProduct />);
        expect(
            screen.getByPlaceholderText('Search products...')
        ).toBeInTheDocument();
        expect(screen.getByLabelText('Search')).toBeInTheDocument();
    });

    it('updates the input value when typing', () => {
        render(<SearchProduct />);
        const input = screen.getByPlaceholderText('Search products...');
        fireEvent.change(input, { target: { value: 'Laptop' } });
        expect(input).toHaveValue('Laptop');
    });
    it('generates correct href on search link', async () => {
        render(<SearchProduct />);

        const input = screen.getByPlaceholderText(
            'Search products...'
        ) as HTMLInputElement;
        const button = screen.getByLabelText('Search');

        fireEvent.change(input, { target: { value: 'eggs' } });

        expect(button).toHaveAttribute(
            'href',
            expect.stringContaining('/search?product=eggs')
        );
    });

    it('trims whitespace from the search query before navigating', () => {
        render(<SearchProduct />);
        const input = screen.getByPlaceholderText('Search products...');
        const button = screen.getByLabelText('Search');

        fireEvent.change(input, { target: { value: '  eggs  ' } });
        fireEvent.click(button);

        expect(button).toHaveAttribute(
            'href',
            expect.stringContaining('/search?product=eggs')
        );
    });
});
