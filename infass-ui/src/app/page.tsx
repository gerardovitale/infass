import { Navbar } from '@/components/Navbar/Navbar';
import { Home } from '@/components/Home/Home';

export default function IndexPage() {
    return (
        <>
            <Navbar />
            <main>
                <Home />
            </main>
        </>
    );
}
