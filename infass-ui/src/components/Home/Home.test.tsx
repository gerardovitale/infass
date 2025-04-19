import '@testing-library/jest-dom';

import React from 'react';
import { render, screen } from '@testing-library/react';
import { Home } from './Home';

describe('Home Component', () => {
    it('renders the heading', () => {
        render(<Home />);
        const heading = screen.getByRole('heading', { name: /Who We Are/i });
        expect(heading).toBeInTheDocument();
    });

    it('renders the description paragraph', () => {
        render(<Home />);
        const paragraph = screen.getByText(
            /PriceTracker is a simple tool to help you monitor the price/i
        );
        expect(paragraph).toBeInTheDocument();
    });
});
