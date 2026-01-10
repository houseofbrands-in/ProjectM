"use client";

import * as React from "react";
import { format } from "date-fns";
import { Calendar as CalendarIcon } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Calendar } from "@/components/ui/calendar";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";

function toISODate(d: Date) {
  // YYYY-MM-DD
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

export function DateRangeBar(props: {
  start: string;
  end: string;
  onApply: (next: { start: string; end: string }) => void;
}) {
  const [startDate, setStartDate] = React.useState<Date>(() => new Date(props.start));
  const [endDate, setEndDate] = React.useState<Date>(() => new Date(props.end));

  return (
    <div className="flex flex-wrap items-center gap-2">
      {/* Start */}
      <Popover>
        <PopoverTrigger asChild>
          <Button variant="outline" className="justify-start rounded-xl">
            <CalendarIcon className="mr-2 h-4 w-4" />
            Start: {format(startDate, "dd MMM yyyy")}
          </Button>
        </PopoverTrigger>
        <PopoverContent className="w-auto p-0" align="start">
          <Calendar mode="single" selected={startDate} onSelect={(d) => d && setStartDate(d)} />
        </PopoverContent>
      </Popover>

      {/* End */}
      <Popover>
        <PopoverTrigger asChild>
          <Button variant="outline" className="justify-start rounded-xl">
            <CalendarIcon className="mr-2 h-4 w-4" />
            End: {format(endDate, "dd MMM yyyy")}
          </Button>
        </PopoverTrigger>
        <PopoverContent className="w-auto p-0" align="start">
          <Calendar mode="single" selected={endDate} onSelect={(d) => d && setEndDate(d)} />
        </PopoverContent>
      </Popover>

      <Button
        className="rounded-xl"
        onClick={() => props.onApply({ start: toISODate(startDate), end: toISODate(endDate) })}
      >
        Apply
      </Button>
    </div>
  );
}
