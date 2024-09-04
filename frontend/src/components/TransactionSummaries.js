import React from 'react';
import { Grid, Card, CardContent, Typography, CardActionArea } from '@mui/material';
import { useNavigate } from 'react-router-dom';

const p_descriptions = [
    'Fulfillment Fee Refund',
    'FBA Inventory Reimbursement - Customer Return',
    'FBA Inventory Reimbursement - Customer Service Issue',
    'FBA Inventory Reimbursement - Damaged:Warehouse',
    'FBA Removal Order: Return Fee',
    'FBA Inbound Pickup Service',
    'Cost of Advertising'
];

export default function TransactionSummaries({ data }) {
  const navigate = useNavigate();

  const handleCardClick = (description) => {
    navigate(`/transaction/${encodeURIComponent(description)}`);
  };

  const getCount = (description) => {
    return data.filter(item => item.P_Description === description).length;
  };

  return (
    <Grid container spacing={2} sx={{ mt: 4, mb: 4 }}>
      {p_descriptions.map((description) => (
        <Grid item xs={12} sm={6} md={4} key={description}>
          <Card>
            <CardActionArea onClick={() => handleCardClick(description)}>
              <CardContent>
                <Typography variant="h6" component="div">
                  {description}
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  Count: {getCount(description)}
                </Typography>
              </CardContent>
            </CardActionArea>
          </Card>
        </Grid>
      ))}
    </Grid>
  );
}