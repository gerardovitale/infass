type LogEntry = {
    severity: string;
    message: string;
    time: string;
    stack_trace?: string;
};

function formatError(error?: Error): Pick<LogEntry, 'stack_trace'> {
    if (!error) return {};
    const parts = [error.stack ?? error.message];
    if (error.cause) {
        parts.push(`Cause: ${JSON.stringify(error.cause)}`);
    }
    return { stack_trace: parts.join('\n') };
}

function log(severity: string, message: string, error?: Error) {
    if (process.env.NODE_ENV === 'production') {
        const entry: LogEntry = {
            severity,
            message,
            time: new Date().toISOString(),
            ...formatError(error),
        };
        console.log(JSON.stringify(entry));
    } else {
        const args: unknown[] = [message];
        if (error) args.push(error);
        if (severity === 'ERROR') {
            console.error(...args);
        } else if (severity === 'WARNING') {
            console.warn(...args);
        } else {
            console.log(...args);
        }
    }
}

export const logger = {
    info: (message: string) => log('INFO', message),
    warn: (message: string) => log('WARNING', message),
    error: (message: string, error?: Error) => log('ERROR', message, error),
};
