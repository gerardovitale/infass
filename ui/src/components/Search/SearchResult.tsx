import { getClient } from '@/auth/getClient';
import { logger } from '@/lib/logger';
import { ProductResponse } from '@/types';
import NoResults from '../ProductList/NoResults';
import InfiniteProductList from '../ProductList/InfiniteProductList';

const SEARCH_LIMIT = 20;

type Props = {
    productSearched: string;
};

export default async function SearchResult(props: Props) {
    try {
        const client = await getClient();
        const res = await client.fetch(
            `${process.env.API_BASE_URL}/products/search?search_term=${props.productSearched}&limit=${SEARCH_LIMIT}&offset=0`
        );
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
        logger.error('Error fetching search results', error instanceof Error ? error : undefined);
        return <NoResults />;
    }
}
