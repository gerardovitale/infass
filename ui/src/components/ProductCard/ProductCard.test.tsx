import '@testing-library/jest-dom';

import { Product } from '@/types';
import { render, screen } from '@testing-library/react';
import { ProductCard } from './ProductCard';

describe('ProductCard', () => {
    const mockProduct: Product = {
        id: '1',
        name: 'Milk',
        size: '1L',
        categories: 'Test Category',
        current_price: 19.9923,
    };

    it('renders the product name', () => {
        render(<ProductCard product={mockProduct} />);
        expect(screen.getByText('Milk')).toBeInTheDocument();
    });

    it('renders the product size', () => {
        render(<ProductCard product={mockProduct} />);
        expect(screen.getByText('1L')).toBeInTheDocument();
    });

    it('renders the product price formatted as currency', () => {
        render(<ProductCard product={mockProduct} />);
        expect(screen.getByText('€19.99')).toBeInTheDocument();
    });

    it('applies correct styles to elements', () => {
        render(<ProductCard product={mockProduct} />);
        const nameElement = screen.getByText('Milk');
        const priceElement = screen.getByText('€19.99');

        expect(nameElement).toHaveClass('text-lg font-semibold text-gray-900');
        expect(priceElement).toHaveClass('text-blue-600 font-bold text-lg');
    });
});
