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

// Example mock data
const data = [
    { date: 'Jan 2023', max_available: 4200, sma15: 2100, sma30: 1400 },
    { date: 'Feb 2023', max_available: 3500, sma15: 2000, sma30: 1350 },
    { date: 'Mar 2023', max_available: 5000, sma15: 2500, sma30: 1500 },
    { date: 'Apr 2023', max_available: 4800, sma15: 2400, sma30: 1450 },
    { date: 'May 2023', max_available: 4700, sma15: 2400, sma30: 1450 },
    { date: 'Jun 2023', max_available: 5000, sma15: 2800, sma30: 1450 },
    { date: 'Jul 2023', max_available: 5100, sma15: 2400, sma30: 1450 },
];

export const ProductDetailChart = () => {
    const [showSMA15, setShowSMA15] = useState(true);
    const [showSMA30, setShowSMA30] = useState(true);

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
                    dataKey="max_available"
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
