'use client';

import { useCallback, useEffect, useRef, useState } from 'react';
import { Product } from '@/types';
import { ProductList } from './ProductList';
import { Spinner } from '@/components/Spinner/Spinner';

type Props = {
    initialProducts: Product[];
    initialHasMore: boolean;
    searchTerm: string;
    limit: number;
};

export default function InfiniteProductList({ initialProducts, initialHasMore, searchTerm, limit }: Props) {
    const [products, setProducts] = useState<Product[]>(initialProducts);
    const [hasMore, setHasMore] = useState(initialHasMore);
    const [isLoading, setIsLoading] = useState(false);
    const [offset, setOffset] = useState(limit);
    const sentinelRef = useRef<HTMLDivElement>(null);

    useEffect(() => {
        setProducts(initialProducts);
        setHasMore(initialHasMore);
        setOffset(limit);
    }, [searchTerm, initialProducts, initialHasMore, limit]);

    const loadMore = useCallback(async () => {
        if (isLoading || !hasMore) return;
        setIsLoading(true);
        try {
            const res = await fetch(
                `/api/search?search_term=${encodeURIComponent(searchTerm)}&limit=${limit}&offset=${offset}`
            );
            const data = await res.json();
            setProducts((prev) => [...prev, ...data.results]);
            setHasMore(data.has_more);
            setOffset((prev) => prev + limit);
        } finally {
            setIsLoading(false);
        }
    }, [isLoading, hasMore, searchTerm, limit, offset]);

    useEffect(() => {
        const sentinel = sentinelRef.current;
        if (!sentinel) return;

        const observer = new IntersectionObserver(
            (entries) => {
                if (entries[0].isIntersecting) {
                    loadMore();
                }
            },
            { rootMargin: '200px' }
        );
        observer.observe(sentinel);
        return () => observer.disconnect();
    }, [loadMore]);

    return (
        <>
            <ProductList products={products} />
            {hasMore && (
                <div ref={sentinelRef}>
                    {isLoading && <Spinner />}
                </div>
            )}
        </>
    );
}
