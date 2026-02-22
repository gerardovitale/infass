type Props = {
    size?: 'sm' | 'md' | 'lg';
};

const sizeClasses = {
    sm: 'h-6 w-6 border-2',
    md: 'h-8 w-8 border-4',
    lg: 'h-12 w-12 border-4',
};

export const Spinner = ({ size = 'md' }: Props) => {
    return (
        <div className="flex justify-center items-center py-8" role="status" aria-label="Loading">
            <div className={`${sizeClasses[size]} animate-spin rounded-full border-gray-300 border-t-blue-500`} />
        </div>
    );
};
