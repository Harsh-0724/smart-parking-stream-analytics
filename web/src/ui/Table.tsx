import type { ReactNode } from "react";
import { Link } from "react-router-dom";

export interface Column<T> {
  key: string;
  header: string;
  align?: "left" | "right";
  width?: string;
  mono?: boolean;
  render: (row: T) => ReactNode;
}

interface Props<T> {
  columns: Column<T>[];
  rows: T[];
  rowKey: (row: T) => string;
  /** Makes the first cell a link that covers the whole row. */
  rowHref?: (row: T) => string;
  caption: string;
}

export function Table<T>({ columns, rows, rowKey, rowHref, caption }: Props<T>) {
  return (
    <div className="table-wrap">
      <table className="table">
        <caption className="sr-only">{caption}</caption>
        <thead>
          <tr>
            {columns.map((c) => (
              <th key={c.key} scope="col" className={c.align === "right" ? "is-right" : ""} style={{ width: c.width }}>
                {c.header}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={rowKey(row)} className={rowHref ? "is-link" : ""}>
              {columns.map((c, i) => (
                <td key={c.key} className={`${c.align === "right" ? "is-right " : ""}${c.mono ? "num" : ""}`}>
                  {i === 0 && rowHref ? (
                    <Link to={rowHref(row)} className="table__rowlink">
                      {c.render(row)}
                    </Link>
                  ) : (
                    c.render(row)
                  )}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
