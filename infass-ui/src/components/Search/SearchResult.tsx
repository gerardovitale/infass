import NoResults from '../ProductList/NoResults';
import { ProductList } from '../ProductList/ProductList';

type Props = {
    productSearched: string;
};

export default async function SearchResult(props: Props) {
    const data = await fetch(
        `${process.env.API_BASE_URL}/products/search?search_term=${props.productSearched}`
    );
    try {
        const response = await data.json();
        if (!response || response.total_results === 0) {
            return <NoResults />;
        }

        return (
            response.total_results !== 0 && (
                <ProductList products={response.results} />
            )
        );
    } catch (error) {
        console.error('Error fetching search results:', error);
        return <NoResults />;
    }
}
