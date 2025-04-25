import { ProductList } from '../ProductList/ProductList';

type Props = {
    productSearched: string;
};

export default async function SearchResult(props: Props) {
    const data = await fetch(
        `${process.env.GET_PRODUCTS_URL}?search=${props.productSearched}`
    );
    const products = await data.json();

    return <ProductList products={products} />;
}
