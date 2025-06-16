import '@testing-library/jest-dom';

import { Product } from '@/types';
import { render, screen } from '@testing-library/react';
import { ProductList } from './ProductList';

describe('ProductList', () => {
    const mockProducts: Product[] = [
        {
            id: '1',
            name: 'Product 1',
            size: 'Large',
            current_price: 19.99,
            categories: 'Category 1',
        },
        {
            id: '2',
            name: 'Product 2',
            size: 'Medium',
            current_price: 9.99,
            categories: 'Category 2',
        },
    ];

    it('renders a list of products', () => {
        render(<ProductList products={mockProducts} />);

        expect(screen.getByText('Product 1')).toBeInTheDocument();
        expect(screen.getByText('Large')).toBeInTheDocument();
        expect(screen.getByText('€19.99')).toBeInTheDocument();

        expect(screen.getByText('Product 2')).toBeInTheDocument();
        expect(screen.getByText('Medium')).toBeInTheDocument();
        expect(screen.getByText('€9.99')).toBeInTheDocument();
    });

    it('renders an empty list when no products are provided', () => {
        render(<ProductList products={[]} />);

        expect(screen.queryByRole('listitem')).not.toBeInTheDocument();
    });

    it('applies correct styles to product items', () => {
        render(<ProductList products={mockProducts} />);

        const productItems = screen.getAllByRole('listitem');
        productItems.forEach((item) => {
            expect(item).toHaveClass(
                'border border-gray-200 rounded-xl p-4 shadow-sm hover:shadow-md transition'
            );
        });
    });
});
