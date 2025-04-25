import { ReadonlyURLSearchParams } from 'next/navigation';

const decodeSearch = (search: ReadonlyURLSearchParams): string => {
    return decodeURIComponent(search.get('product') || '');
};

export { decodeSearch };
