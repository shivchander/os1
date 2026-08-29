import * as React from "react";
import { createFileRoute } from "@tanstack/react-router";

import { ViewLoadingFallback } from "@/shared/ui/ViewLoadingFallback";

const NodesPanel = React.lazy(async () => {
  const module = await import("@/features/nodes/ui/NodesPanel");
  return { default: module.NodesPanel };
});

export const Route = createFileRoute("/nodes")({
  component: NodesRouteComponent,
});

function NodesRouteComponent() {
  return (
    <React.Suspense fallback={<ViewLoadingFallback kind="workflows" />}>
      <NodesPanel />
    </React.Suspense>
  );
}
