import '@testing-library/jest-dom';
import { render, screen } from '@testing-library/react';
import { Spinner } from './Spinner';

describe('Spinner', () => {
    it('renders with status role and loading label', () => {
        render(<Spinner />);
        const spinner = screen.getByRole('status', { name: 'Loading' });
        expect(spinner).toBeInTheDocument();
    });

    it('renders with default md size', () => {
        const { container } = render(<Spinner />);
        const circle = container.querySelector('.animate-spin');
        expect(circle).toHaveClass('h-8', 'w-8', 'border-4');
    });

    it('renders with sm size', () => {
        const { container } = render(<Spinner size="sm" />);
        const circle = container.querySelector('.animate-spin');
        expect(circle).toHaveClass('h-6', 'w-6', 'border-2');
    });

    it('renders with lg size', () => {
        const { container } = render(<Spinner size="lg" />);
        const circle = container.querySelector('.animate-spin');
        expect(circle).toHaveClass('h-12', 'w-12', 'border-4');
    });
});
