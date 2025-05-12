export type Product = {
    id: string;
    name: string;
    size: string;
    price: number;
    category: string;
    priceDetails: PriceDetail[];
};

export type PriceDetail = {
    date: string;
    max_available: number;
    sma15: number;
    sma30: number;
};
