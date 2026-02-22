'use client';

import {
    LineChart,
    Line,
    XAxis,
    YAxis,
    Tooltip,
    Legend,
    CartesianGrid,
} from 'recharts';
import { useState } from 'react';
import { PriceDetail } from '@/types';

type Props = {
    data: PriceDetail[];
};

export const ProductDetailChart = ({ data }: Props) => {
    const [showSMA15, setShowSMA15] = useState(false);
    const [showSMA30, setShowSMA30] = useState(false);

    return (
        <div className="p-4 border rounded-xl bg-white shadow-md">
            <div className="flex justify-end gap-4 mb-2">
                <label className="flex items-center gap-1">
                    <input
                        type="checkbox"
                        checked={showSMA15}
                        onChange={() => setShowSMA15((prev) => !prev)}
                    />
                    <span
                        className="w-3 h-3 rounded-sm inline-block"
                        style={{ backgroundColor: '#06b6d4' }}
                    />
                    <span>SMA 15</span>
                </label>
                <label className="flex items-center gap-1">
                    <input
                        type="checkbox"
                        checked={showSMA30}
                        onChange={() => setShowSMA30((prev) => !prev)}
                    />
                    <span
                        className="w-3 h-3 rounded-sm inline-block"
                        style={{ backgroundColor: '#e11d48' }}
                    />
                    <span>SMA 30</span>
                </label>
            </div>
            <LineChart data={data} width={700} height={400}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="date" />
                <YAxis />
                <Tooltip />
                <Legend />
                <Line
                    type="monotone"
                    dataKey="price"
                    stroke="#2563eb"
                    strokeWidth={2}
                    dot={false}
                />
                {showSMA15 && (
                    <Line
                        type="monotone"
                        dataKey="sma15"
                        stroke="#06b6d4"
                        strokeWidth={2}
                        dot={false}
                    />
                )}
                {showSMA30 && (
                    <Line
                        type="monotone"
                        dataKey="sma30"
                        stroke="#e11d48"
                        strokeWidth={2}
                        dot={false}
                    />
                )}
            </LineChart>
        </div>
    );
};
