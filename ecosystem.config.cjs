module.exports = {
  apps: [
    {
      name: "stock-tradebot-watch",
      cwd: "/Users/alanguo/Projects/stock-trader",
      script: "./scripts/run-watch.sh",
      interpreter: "bash",
      autorestart: true,
      restart_delay: 5000,
      time: true,
      env: {
        WATCH_CONFIG_DIR: "config/oneshot-tasks",
        WATCH_POLL_SECONDS: "5",
      },
    },
  ],
};
