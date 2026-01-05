import React, { useState } from 'react';
import {
  Box,
  Typography,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  Chip,
  TablePagination,
  Alert,
  Card,
  CardContent,
  Grid,
} from '@mui/material';

interface LineageResult {
  view_name: string;
  view_column: string;
  source_table: string;
  source_column: string;
  confidence_score: number;
  column_type?: string;
  expression_type?: string;
}

interface ResultsSummary {
  job_info: {
    total_results: number;
    processing_time_seconds?: number;
  };
  confidence_statistics: {
    average: number;
    minimum: number;
    maximum: number;
  };
  column_type_distribution: Record<string, number>;
  success_rate: {
    view_success_rate: number;
    high_confidence_results: number;
  };
}

interface ResultsViewerProps {
  results: LineageResult[];
  summary?: ResultsSummary;
  totalResults: number;
}

const ResultsViewer: React.FC<ResultsViewerProps> = ({
  results,
  summary,
  totalResults,
}) => {
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(25);

  const handleChangePage = (event: unknown, newPage: number) => {
    setPage(newPage);
  };

  const handleChangeRowsPerPage = (event: React.ChangeEvent<HTMLInputElement>) => {
    setRowsPerPage(parseInt(event.target.value, 10));
    setPage(0);
  };

  const getConfidenceColor = (score: number) => {
    if (score >= 0.8) return 'success';
    if (score >= 0.6) return 'warning';
    return 'error';
  };

  const formatConfidence = (score: number) => {
    return `${(score * 100).toFixed(1)}%`;
  };

  return (
    <Box>
      <Typography variant="h6" gutterBottom>
        Analysis Results
      </Typography>

      {/* Summary Cards */}
      {summary && (
        <Grid container spacing={2} sx={{ mb: 3 }}>
          <Grid item xs={12} sm={6} md={3}>
            <Card>
              <CardContent>
                <Typography variant="h4" color="primary">
                  {summary.job_info.total_results}
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  Total Results
                </Typography>
              </CardContent>
            </Card>
          </Grid>
          
          <Grid item xs={12} sm={6} md={3}>
            <Card>
              <CardContent>
                <Typography variant="h4" color="success.main">
                  {summary.confidence_statistics.average.toFixed(1)}%
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  Avg Confidence
                </Typography>
              </CardContent>
            </Card>
          </Grid>
          
          <Grid item xs={12} sm={6} md={3}>
            <Card>
              <CardContent>
                <Typography variant="h4" color="info.main">
                  {summary.success_rate.view_success_rate.toFixed(1)}%
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  View Success Rate
                </Typography>
              </CardContent>
            </Card>
          </Grid>
          
          <Grid item xs={12} sm={6} md={3}>
            <Card>
              <CardContent>
                <Typography variant="h4" color="warning.main">
                  {summary.success_rate.high_confidence_results.toFixed(1)}%
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  High Confidence
                </Typography>
              </CardContent>
            </Card>
          </Grid>
        </Grid>
      )}

      {results.length === 0 ? (
        <Alert severity="info">
          No lineage results found.
        </Alert>
      ) : (
        <>
          <TableContainer component={Paper}>
            <Table>
              <TableHead>
                <TableRow>
                  <TableCell>View</TableCell>
                  <TableCell>View Column</TableCell>
                  <TableCell>Source Table</TableCell>
                  <TableCell>Source Column</TableCell>
                  <TableCell>Confidence</TableCell>
                  <TableCell>Type</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {results
                  .slice(page * rowsPerPage, page * rowsPerPage + rowsPerPage)
                  .map((result, index) => (
                    <TableRow key={index}>
                      <TableCell>
                        <Typography variant="body2" fontWeight="medium">
                          {result.view_name}
                        </Typography>
                      </TableCell>
                      <TableCell>
                        <Typography variant="body2">
                          {result.view_column}
                        </Typography>
                      </TableCell>
                      <TableCell>
                        <Typography variant="body2">
                          {result.source_table}
                        </Typography>
                      </TableCell>
                      <TableCell>
                        <Typography variant="body2">
                          {result.source_column}
                        </Typography>
                      </TableCell>
                      <TableCell>
                        <Chip
                          label={formatConfidence(result.confidence_score)}
                          color={getConfidenceColor(result.confidence_score)}
                          size="small"
                        />
                      </TableCell>
                      <TableCell>
                        {result.column_type && (
                          <Chip
                            label={result.column_type}
                            variant="outlined"
                            size="small"
                          />
                        )}
                      </TableCell>
                    </TableRow>
                  ))}
              </TableBody>
            </Table>
          </TableContainer>

          <TablePagination
            rowsPerPageOptions={[10, 25, 50, 100]}
            component="div"
            count={totalResults}
            rowsPerPage={rowsPerPage}
            page={page}
            onPageChange={handleChangePage}
            onRowsPerPageChange={handleChangeRowsPerPage}
          />
        </>
      )}
    </Box>
  );
};

export default ResultsViewer;