import React, { useState } from 'react';
import { Button, Grid, Typography, CircularProgress } from '@mui/material';
import CloudUploadIcon from '@mui/icons-material/CloudUpload';

export default function FileUpload({ onDataProcessed }) {
  const [mtrFile, setMtrFile] = useState(null);
  const [paymentFile, setPaymentFile] = useState(null);
  const [loading, setLoading] = useState(false);

  const handleFileChange = (event, fileType) => {
    const file = event.target.files[0];
    if (fileType === 'mtr') {
      setMtrFile(file);
    } else {
      setPaymentFile(file);
    }
  };

  const handleUpload = async () => {
    if (!mtrFile || !paymentFile) {
      alert('Please select both MTR and Payment files');
      return;
    }

    setLoading(true);

    const formData = new FormData();
    formData.append('mtr_file', mtrFile);
    formData.append('payment_file', paymentFile);

    try {
      const response = await fetch('/api/process_csv', {
        method: 'POST',
        body: formData,
      });

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      const data = await response.json();
      onDataProcessed(data);
    } catch (error) {
      console.error('Error:', error);
      alert('An error occurred while processing the files');
    } finally {
      setLoading(false);
    }
  };

  return (
    <Grid container spacing={2} alignItems="center">
      <Grid item xs={12} sm={4}>
        <input
          accept=".csv"
          style={{ display: 'none' }}
          id="mtr-file"
          type="file"
          onChange={(e) => handleFileChange(e, 'mtr')}
        />
        <label htmlFor="mtr-file">
          <Button variant="contained" component="span" startIcon={<CloudUploadIcon />}>
            Upload MTR
          </Button>
        </label>
        {mtrFile && <Typography variant="body2">{mtrFile.name}</Typography>}
      </Grid>
      <Grid item xs={12} sm={4}>
        <input
          accept=".csv"
          style={{ display: 'none' }}
          id="payment-file"
          type="file"
          onChange={(e) => handleFileChange(e, 'payment')}
        />
        <label htmlFor="payment-file">
          <Button variant="contained" component="span" startIcon={<CloudUploadIcon />}>
            Upload Payment
          </Button>
        </label>
        {paymentFile && <Typography variant="body2">{paymentFile.name}</Typography>}
      </Grid>
      <Grid item xs={12} sm={4}>
        <Button
          variant="contained"
          color="primary"
          onClick={handleUpload}
          disabled={!mtrFile || !paymentFile || loading}
        >
          {loading ? <CircularProgress size={24} /> : 'Process Files'}
        </Button>
      </Grid>
    </Grid>
  );
}