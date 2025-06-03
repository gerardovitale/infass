import NoResults from '../ProductList/NoResults';
import { ProductList } from '../ProductList/ProductList';

type Props = {
    productSearched: string;
};

export default async function SearchResult(props: Props) {
    const data = await fetch(
        `${process.env.GET_PRODUCTS_URL}?search_term=${props.productSearched}`
    );
    const response = await data.json();
    if (!response || response.total_results === 0) {
        return <NoResults/>;
    }

    return response.total_results !== 0 && (<ProductList products={response.results} />) ;
}
