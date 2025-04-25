'use client';

import { useSearchParams } from 'next/navigation';
import { ProductList } from '../ProductList/ProductList';
import { Product } from '@/types';
import { decodeSearch } from './utils/searchUtils';

const SearchResult = () => {
    const searchParams = useSearchParams();
    const productSearched = decodeSearch(searchParams);
    const products = [
        {
            id: '1',
            name: 'Product 1',
            size: 'Size M',
            price: 19.99,
            category: 'Category 1',
        },
        {
            id: '2',
            name: 'Product 2',
            size: 'Size L',
            price: 29.99,
            category: 'Category 1',
        },
        {
            id: '3',
            name: 'Product 3',
            size: 'Size S',
            price: 39.99,
            category: 'Category 1',
        },
    ];

    const filteredProducts = (searchedValue: string): Product[] => {
        return products.filter((p) =>
            p.name.toLowerCase().includes(searchedValue.toLowerCase())
        );
    };
    return <ProductList products={filteredProducts(productSearched)} />;
};

export default SearchResult;
