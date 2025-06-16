export type Product = {
    id: string;
    name: string;
    size: string;
    current_price: number;
    categories: string;
    image_url?: string;
    price_details?: PriceDetail[];
};

export type PriceDetail = {
    date: string;
    price: number;
    sma15: number;
    sma30: number;
};

export type ProductResponse = {
    results: Product[];
    total_results: number;
};
