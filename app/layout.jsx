import './globals.css';

export const metadata = {
  title: 'Crypto Intelligence Platform | Data Engineering & Analytics',
  description: 'End-to-End Crypto Market Trends and Algorithmic Trading Signal Intelligence Platform powered by Python ETL, Airflow DAG, PostgreSQL Star Schema, and MongoDB Lakehouse.'
};

export default function RootLayout({ children }) {
  return (
    <html lang="en" className="dark-theme" style={{ backgroundColor: '#07090e', color: '#f3f4f6' }}>
      <head>
        <link rel="preconnect" href="https://fonts.googleapis.com" />
        <link rel="preconnect" href="https://fonts.gstatic.com" crossOrigin="anonymous" />
        <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;700&family=Outfit:wght@300;400;500;600;700;800&display=swap" rel="stylesheet" />
      </head>
      <body className="dark-theme" style={{ backgroundColor: '#07090e', color: '#f3f4f6', margin: 0, padding: 0 }}>
        {children}
      </body>
    </html>
  );
}
