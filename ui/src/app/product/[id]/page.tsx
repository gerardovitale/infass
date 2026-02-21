import { getClient } from '@/auth/getClient';
import { ProductDetail } from '@/components/ProductDetail/ProductDetail';
import { Product } from '@/types';
import { notFound } from 'next/navigation';
import { PageProps } from '../../../../.next/types/app/layout';

export default async function ProductPage({ params }: PageProps) {
    const { id } = await params;
    const URL = `${process.env.API_BASE_URL}/products/${id}`;
    let res;
    try {
        const client = await getClient();
        res = await client.fetch(URL);
    } catch (error) {
        const cause = error instanceof Error ? error.cause : undefined;
        console.error('Failed to fetch product:', error, 'cause:', JSON.stringify(cause));
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
    const product = res.data as Product;
    return <ProductDetail product={product} />;
}
