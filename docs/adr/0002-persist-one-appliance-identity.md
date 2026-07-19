# Persist one appliance identity

Beacon generates its experimental ISO 11783 identity number once in SQLite and reuses the resulting appliance NAME across restarts and independently connected bus endpoints. Assigned manufacturer/product codes and certification claims remain explicit configuration concerns rather than values Beacon fabricates, so development builds are stable without impersonating a certified product.
