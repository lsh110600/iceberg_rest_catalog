import { Suspense } from "react";
import { OperationsClient } from "@/components/OperationsClient";
import { api } from "@/lib/api";

export const dynamic = "force-dynamic";

export default async function OperationsPage() {
  const [operationPage, tablePage, worker, catalogs] = await Promise.all([
    api.operationPage(),
    api.tablePage(),
    api.sparkWorker(),
    api.playgroundCatalogs(),
  ]);
  return (
    <div className="page-shell">
      <section className="page-heading"><div><p className="eyebrow">CONTROLLED EXECUTION</p><h1>Operations</h1><p className="lede">계획과 승인을 거쳐 Iceberg maintenance 작업을 요청합니다.</p></div></section>
      <Suspense fallback={<div className="panel">작업 화면을 준비하는 중입니다.</div>}>
        <OperationsClient
          initialOperations={operationPage.items}
          initialOperationCursor={operationPage.nextCursor}
          initialHasMoreOperations={operationPage.hasMore}
          initialTables={tablePage.items}
          initialTableCursor={tablePage.nextCursor}
          initialHasMoreTables={tablePage.hasMore}
          initialWorker={worker}
          initialCatalogs={catalogs}
        />
      </Suspense>
    </div>
  );
}
