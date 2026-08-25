import { PlaygroundClient } from "@/components/PlaygroundClient";
import { api } from "@/lib/api";

export const dynamic = "force-dynamic";

export default async function PlaygroundPage() {
  const [status, catalogs] = await Promise.all([
    api.playgroundStatus(),
    api.playgroundCatalogs(),
  ]);
  return (
    <div className="page-shell playground-page">
      <section className="page-heading">
        <div>
          <p className="eyebrow">LOCAL LAKEHOUSE LAB</p>
          <h1>Playground</h1>
          <p className="lede">HDFS의 Iceberg table을 선택하고 Spark에서 metadata를 직접 조회합니다.</p>
        </div>
        <div className="playground-warning">LOCAL TEST ONLY</div>
      </section>
      <PlaygroundClient initialStatus={status} initialCatalogs={catalogs} />
    </div>
  );
}
