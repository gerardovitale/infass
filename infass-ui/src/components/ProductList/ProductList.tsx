import { Product } from '@/types';

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
                        <div className="flex items-center justify-between">
                            <div>
                                <h3 className="text-lg font-semibold text-gray-900">
                                    {product.name}
                                </h3>
                                <p className="text-sm text-gray-500">
                                    {product.size}
                                </p>
                            </div>
                            <span className="text-blue-600 font-bold text-lg">
                                €{product.price.toFixed(2)}
                            </span>
                        </div>
                    </li>
                ))}
            </ul>
        </main>
    );
};
