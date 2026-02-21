import { getClient } from '@/auth/getClient';
import { ProductResponse } from '@/types';
import NoResults from '../ProductList/NoResults';
import InfiniteProductList from '../ProductList/InfiniteProductList';

const SEARCH_LIMIT = 20;

type Props = {
    productSearched: string;
};

export default async function SearchResult(props: Props) {
    console.log('About to fetch search results for:', props.productSearched);
    console.log('API Base URL:', process.env.API_BASE_URL);

    try {
        const client = await getClient();
        const res = await client.fetch(
            `${process.env.API_BASE_URL}/products/search?search_term=${props.productSearched}&limit=${SEARCH_LIMIT}&offset=0`
        );
        console.log('Fetch response:', res);
        const response = res.data as ProductResponse;
        if (!response || response.total_results === 0) {
            return <NoResults />;
        }

        return (
            <InfiniteProductList
                initialProducts={response.results}
                initialHasMore={response.has_more}
                searchTerm={props.productSearched}
                limit={SEARCH_LIMIT}
            />
        );
    } catch (error) {
        console.error('Error fetching search results:', error);
        return <NoResults />;
    }
}
