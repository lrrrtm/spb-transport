import { Component, type ReactNode } from "react";
import Alert from "@mui/material/Alert";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";

interface MapErrorBoundaryProps {
  children: ReactNode;
}

interface MapErrorBoundaryState {
  failed: boolean;
  resetKey: number;
}

export class MapErrorBoundary extends Component<MapErrorBoundaryProps, MapErrorBoundaryState> {
  state: MapErrorBoundaryState = { failed: false, resetKey: 0 };

  static getDerivedStateFromError(): Partial<MapErrorBoundaryState> {
    return { failed: true };
  }

  retry = () => {
    this.setState(({ resetKey }) => ({ failed: false, resetKey: resetKey + 1 }));
  };

  render() {
    if (this.state.failed) {
      return (
        <Alert
          severity="error"
          action={
            <Button color="inherit" size="small" onClick={this.retry}>
              Повторить
            </Button>
          }
          sx={{ alignSelf: "flex-start", m: 3 }}
        >
          Не удалось открыть карту
        </Alert>
      );
    }
    return (
      <Box key={this.state.resetKey} sx={{ display: "contents" }}>
        {this.props.children}
      </Box>
    );
  }
}
