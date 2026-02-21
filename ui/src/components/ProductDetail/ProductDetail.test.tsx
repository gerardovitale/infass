import '@testing-library/jest-dom';

import { Product } from '@/types';
import { render, screen } from '@testing-library/react';
import { ProductDetail } from './ProductDetail';

jest.mock('next/navigation', () => ({
    useRouter: () => ({ back: jest.fn() }),
}));

describe('ProductDetail', () => {
    const mockProduct: Product = {
        id: '1',
        name: 'Test Product',
        size: 'Large',
        categories: 'Electronics',
        current_price: 99.99,
        price_details: [],
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
        expect(screen.getByText('No data available')).toBeInTheDocument();
    });

    it('renders chart with data', () => {
        const productWithData: Product = {
            ...mockProduct,
            price_details: [
                {
                    date: '2023-01-01',
                    price: 100,
                    sma15: 90,
                    sma30: 80,
                },
            ],
        };

        render(<ProductDetail product={productWithData} />);

        expect(screen.getByText('SMA 15')).toBeInTheDocument();
        expect(screen.getByText('SMA 30')).toBeInTheDocument();
    });
});
