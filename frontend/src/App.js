import React, { useState } from 'react';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import CssBaseline from '@mui/material/CssBaseline';
import Container from '@mui/material/Container';
import Typography from '@mui/material/Typography';
import Box from '@mui/material/Box';
import { BrowserRouter as Router, Route, Routes } from 'react-router-dom';
import FileUpload from './components/FileUpload';
import DataDisplay from './components/DataDisplay';
import TransactionSummaries from './components/TransactionSummaries';
import TransactionDetails from './components/TransactionDetails';

const theme = createTheme();

export default function App() {
  const [processedData, setProcessedData] = useState(null);

  const handleDataProcessed = (data) => {
    setProcessedData(data);
  };

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <Router>
        <Container maxWidth="lg">
          <Box sx={{ my: 4 }}>
            <Typography variant="h4" component="h1" gutterBottom>
              CSV Processor
            </Typography>
            <FileUpload onDataProcessed={handleDataProcessed} />
            <Routes>
              <Route path="/" element={
                processedData && (
                  <>
                    <TransactionSummaries data={processedData} />
                    <DataDisplay data={processedData} />
                  </>
                )
              } />
              <Route path="/transaction/:description" element={<TransactionDetails data={processedData} />} />
            </Routes>
          </Box>
        </Container>
      </Router>
    </ThemeProvider>
  );
}