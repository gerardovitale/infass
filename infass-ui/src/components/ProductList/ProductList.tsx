import { Product } from '@/types';
import { ProductCard } from '../ProductCard/ProductCard';

type ProductListProps = {
    products: Product[];
};

export const ProductList = (props: ProductListProps) => {
    return (
        <main className="px-50">
            <ul className="space-y-4 mt-6">
                {props.products.map((product) => (
                    <li
                        key={product.id}
                        className="border border-gray-200 rounded-xl p-4 shadow-sm hover:shadow-md transition"
                    >
                        <ProductCard product={product} />
                    </li>
                ))}
            </ul>
        </main>
    );
};
