import { getClient } from '@/auth/getClient';
import { ProductDetail } from '@/components/ProductDetail/ProductDetail';
import { Product } from '@/types';
import { notFound } from 'next/navigation';
import { PageProps } from '../../../../.next/types/app/layout';

export default async function ProductPage({ params }: PageProps) {
    const { id } = await params;
    const URL = `${process.env.API_BASE_URL}/products/${id}`;
    const client = await getClient();
    const res = await client.fetch(URL);
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
