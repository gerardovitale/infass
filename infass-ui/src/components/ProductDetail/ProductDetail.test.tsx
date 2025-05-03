import '@testing-library/jest-dom';

import { render, screen } from '@testing-library/react';
import { ProductDetail } from './ProductDetail';
import { Product } from '@/types';

describe('ProductDetail', () => {
    const mockProduct: Product = {
        id: '1',
        name: 'Test Product',
        size: 'Large',
        category: 'Electronics',
        price: 99.99,
    };

    it('renders product name, size, category, and price', () => {
        render(<ProductDetail product={mockProduct} />);

        expect(screen.getByText('Test Product')).toBeInTheDocument();
        expect(screen.getByText('Large - Electronics')).toBeInTheDocument();
        expect(screen.getByText('99.99€')).toBeInTheDocument();
    });

    it('renders fallback image', () => {
        render(<ProductDetail product={mockProduct} />);

        const image = screen.getByAltText('Test Product') as HTMLImageElement;
        expect(decodeURIComponent(image.src)).toContain(
            '/images/default-image.png'
        );
    });

    it('renders chart placeholder', () => {
        render(<ProductDetail product={mockProduct} />);

        expect(screen.getByText('Chart will go here')).toBeInTheDocument();
    });
});
