import type { SidebarsConfig } from "@docusaurus/plugin-content-docs";

const sidebar: SidebarsConfig = {
  apisidebar: [
    {
      type: "doc",
      id: "api-reference/rest/order-management-api",
    },
    {
      type: "category",
      label: "Orders",
      link: {
        type: "doc",
        id: "api-reference/rest/orders",
      },
      items: [
        {
          type: "doc",
          id: "api-reference/rest/receive-order",
          label: "Receive a new order — allocates and releases automatically",
          className: "api-method post",
        },
        {
          type: "doc",
          id: "api-reference/rest/get-order",
          label: "Read an order's current state",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api-reference/rest/cancel-order",
          label: "Cancel an order and revoke its reservations",
          className: "api-method delete",
        },
      ],
    },
    {
      type: "category",
      label: "Allocation",
      link: {
        type: "doc",
        id: "api-reference/rest/allocation",
      },
      items: [
        {
          type: "doc",
          id: "api-reference/rest/release-held-order",
          label: "Release an order held at intake",
          className: "api-method post",
        },
        {
          type: "doc",
          id: "api-reference/rest/retry-allocation",
          label: "Re-attempt allocation for backordered lines, then release",
          className: "api-method post",
        },
      ],
    },
    {
      type: "category",
      label: "PlannedCapacity",
      link: {
        type: "doc",
        id: "api-reference/rest/planned-capacity",
      },
      items: [
        {
          type: "doc",
          id: "api-reference/rest/get-planned-capacity",
          label: "List a site's planned capacity windows",
          className: "api-method get",
        },
      ],
    },
    {
      type: "category",
      label: "Health",
      link: {
        type: "doc",
        id: "api-reference/rest/health",
      },
      items: [
        {
          type: "doc",
          id: "api-reference/rest/get-healthz",
          label: "Liveness probe",
          className: "api-method get",
        },
      ],
    },
  ],
};

export default sidebar.apisidebar;
