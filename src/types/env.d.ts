declare module 'react' {
    export function useState<T>(initialState: T | (() => T)): [T, (newState: T | ((prevState: T) => T)) => void];
    export function useEffect(effect: () => void | (() => void | undefined), deps?: ReadonlyArray<any>): void;
    export default React;
}

declare module 'recharts';

declare namespace JSX {
    interface IntrinsicElements {
        [elemName: string]: any;
    }
}

declare var process: {
    env: {
        [key: string]: string | undefined;
    };
};
