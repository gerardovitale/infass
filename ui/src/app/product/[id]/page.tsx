import { getClient } from '@/auth/getClient';
import { logger } from '@/lib/logger';
import { ProductDetail } from '@/components/ProductDetail/ProductDetail';
import { Product } from '@/types';
import { notFound } from 'next/navigation';
import { unstable_cache } from 'next/cache';
import { PageProps } from '../../../../.next/types/app/layout';

const CACHE_REVALIDATE_SECONDS = 300;

const fetchProduct = unstable_cache(
    async (productId: string) => {
        const client = await getClient();
        const res = await client.fetch(`${process.env.API_BASE_URL}/products/${productId}`);
        return { status: res.status, data: res.data as Product };
    },
    ['product-detail'],
    { revalidate: CACHE_REVALIDATE_SECONDS }
);

export default async function ProductPage({ params }: PageProps) {
    const { id } = await params;
    let res;
    try {
        res = await fetchProduct(id);
    } catch (error) {
        logger.error('Failed to fetch product', error instanceof Error ? error : undefined);
        throw error;
    }
    if (res.status !== 200) {
        switch (res.status) {
            case 404:
                return notFound();
            default:
                return <div className="p-8">Failed to fetch product</div>;
        }
    }
    return <ProductDetail product={res.data} />;
}
