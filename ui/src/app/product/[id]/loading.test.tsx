import '@testing-library/jest-dom';
import { render, screen } from '@testing-library/react';
import Loading from './loading';

describe('Product Detail Loading', () => {
    it('renders a spinner while product data is loading', () => {
        render(<Loading />);
        const spinner = screen.getByRole('status', { name: 'Loading' });
        expect(spinner).toBeInTheDocument();
    });

    it('renders the large spinner variant', () => {
        const { container } = render(<Loading />);
        const circle = container.querySelector('.animate-spin');
        expect(circle).toHaveClass('h-12', 'w-12');
    });
});
