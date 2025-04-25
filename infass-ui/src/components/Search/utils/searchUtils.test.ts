import { ReadonlyURLSearchParams } from 'next/navigation';
import { decodeSearch } from './searchUtils';

describe('decodeSearch', () => {
    it('should decode the "product" parameter from the search params', () => {
        const mockSearchParams = {
            get: (key: string) => (key === 'product' ? 'test%20product' : null),
        } as unknown as ReadonlyURLSearchParams;

        const result = decodeSearch(mockSearchParams);
        expect(result).toBe('test product');
    });

    it('should return an empty string if "product" parameter is not present', () => {
        const mockSearchParams = {
            get: () => null,
        } as unknown as ReadonlyURLSearchParams;

        const result = decodeSearch(mockSearchParams);
        expect(result).toBe('');
    });

    it('should return an empty string if "product" parameter is an empty string', () => {
        const mockSearchParams = {
            get: (key: string) => (key === 'product' ? '' : null),
        } as unknown as ReadonlyURLSearchParams;

        const result = decodeSearch(mockSearchParams);
        expect(result).toBe('');
    });
});
