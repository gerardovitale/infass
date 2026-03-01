import { formatDateTick } from './ProductDetailChart';

describe('ProductDetailChart', () => {
    describe('formatDateTick', () => {
        it('formats ISO date as "Mon YYYY"', () => {
            expect(formatDateTick('2025-09-02')).toBe('Sep 2025');
        });

        it('formats January date correctly', () => {
            expect(formatDateTick('2024-01-15')).toBe('Jan 2024');
        });

        it('formats December date correctly', () => {
            expect(formatDateTick('2023-12-31')).toBe('Dec 2023');
        });
    });
});
