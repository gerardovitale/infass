import { getClient } from '@/auth/getClient';
import { ProductResponse } from '@/types';
import NoResults from '../ProductList/NoResults';
import { ProductList } from '../ProductList/ProductList';

type Props = {
    productSearched: string;
};

export default async function SearchResult(props: Props) {
    console.log('About to fetch search results for:', props.productSearched);
    console.log('API Base URL:', process.env.API_BASE_URL);

    try {
        const client = await getClient();
        const res = await client.fetch(
            `${process.env.API_BASE_URL}/products/search?search_term=${props.productSearched}`
        );
        console.log('Fetch response:', res);
        const response = res.data as ProductResponse;
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
