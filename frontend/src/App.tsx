import { AnomaliesTable } from "./components/AnomaliesTable";
import { StatusPanel } from "./components/StatusPanel";
import { TickFeed } from "./components/TickFeed";

function App() {
  return (
    <div className="app">
      <header className="app-header">
        <h1>StreamAlpha</h1>
        <p className="subtitle">Real-time market anomaly detection</p>
      </header>
      <main className="dashboard">
        <TickFeed />
        <AnomaliesTable />
        <StatusPanel />
      </main>
    </div>
  );
}

export default App;
