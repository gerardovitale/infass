import '@testing-library/jest-dom';

import { render, screen, fireEvent } from '@testing-library/react';
import { ProductImage } from './ProductImage';

describe('ProductImage', () => {
    it('renders the provided src when it is a valid https URL', () => {
        render(
            <ProductImage
                src="https://prod-mercadona.imgix.net/image.jpg"
                alt="test"
                width={100}
                height={100}
            />
        );
        const img = screen.getByRole('img', { name: 'test' });
        expect(img).toHaveAttribute(
            'src',
            expect.stringContaining('prod-mercadona.imgix.net')
        );
    });

    it('falls back to placeholder when src is undefined', () => {
        render(<ProductImage alt="test" width={100} height={100} />);
        const img = screen.getByRole('img', { name: 'test' });
        expect(img).toHaveAttribute(
            'src',
            expect.stringContaining('default-image.svg')
        );
    });

    it('falls back to placeholder when src is an invalid URL', () => {
        render(
            <ProductImage
                src="ftp://invalid.com/image.jpg"
                alt="test"
                width={100}
                height={100}
            />
        );
        const img = screen.getByRole('img', { name: 'test' });
        expect(img).toHaveAttribute(
            'src',
            expect.stringContaining('default-image.svg')
        );
    });

    it('falls back to placeholder on image load error', () => {
        render(
            <ProductImage
                src="https://prod-mercadona.imgix.net/image.jpg"
                alt="test"
                width={100}
                height={100}
            />
        );
        const img = screen.getByRole('img', { name: 'test' });
        fireEvent.error(img);
        expect(img).toHaveAttribute(
            'src',
            expect.stringContaining('default-image.svg')
        );
    });
});
