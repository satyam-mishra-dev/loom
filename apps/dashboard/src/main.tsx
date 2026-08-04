import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { Toaster } from 'sonner';
import { App } from './App.js';
import './index.css';

const root = document.getElementById('root');
if (root === null) throw new Error('missing #root');
createRoot(root).render(
  <StrictMode>
    <App />
    <Toaster
      position="bottom-center"
      theme="dark"
      toastOptions={{
        style: {
          background: 'rgba(16,22,36,0.95)',
          border: '1px solid #223049',
          color: '#e8ecf4',
          fontFamily: "'Inter', sans-serif",
          borderRadius: '10px',
        },
      }}
    />
  </StrictMode>,
);
