import { useEffect, useState } from 'react';
import { useLang } from '@rspress/core/runtime';

export default function A11yLabels() {
  const language = useLang().startsWith('zh') ? 'zh' : 'en';
  const [mobileSidebarOpen, setMobileSidebarOpen] = useState(false);
  const copy = language === 'zh'
    ? {
        title: 'Batch v2 · 未发布',
        detail: '本站对应当前源码中的 BatchQueue 接口；请安装本地根包，npm 历史版本不适用。',
        closeMenu: '关闭文档菜单'
      }
    : {
        title: 'Batch v2 · Unreleased',
        detail: 'This manual matches the BatchQueue source tree. Install its local root tarball; historical npm releases do not implement this API.',
        closeMenu: 'Close documentation menu'
      };

  useEffect(() => {
    const sidebar = document.querySelector<HTMLElement>('.rp-doc-layout__sidebar');
    const menuButton = document.querySelector<HTMLButtonElement>('.rp-sidebar-menu__left');
    if (!sidebar || !menuButton) return;

    sidebar.id = 'queuebit-docs-sidebar';
    menuButton.setAttribute('aria-controls', sidebar.id);
    menuButton.setAttribute('aria-expanded', String(mobileSidebarOpen));
  }, [language, mobileSidebarOpen]);

  useEffect(() => {
    const mediaQuery = window.matchMedia('(max-width: 768px)');
    const sidebarSelector = '.rp-doc-layout__sidebar';
    const openClass = 'rp-doc-layout__sidebar--open';

    const closeSidebar = () => {
      document.querySelector<HTMLElement>(sidebarSelector)?.classList.remove(openClass);
      setMobileSidebarOpen(false);
    };

    const handleDocumentClick = (event: MouseEvent) => {
      if (!mediaQuery.matches || !(event.target instanceof Element)) return;

      const sidebar = document.querySelector<HTMLElement>(sidebarSelector);
      if (!sidebar) return;

      if (event.target.closest('.rp-sidebar-menu__left')) {
        event.preventDefault();
        event.stopPropagation();
        const shouldOpen = !sidebar.classList.contains(openClass);
        sidebar.classList.toggle(openClass, shouldOpen);
        setMobileSidebarOpen(shouldOpen);
        return;
      }

      if (!sidebar.classList.contains(openClass)) return;

      if (event.target.closest(sidebarSelector)) {
        if (event.target.closest('a')) closeSidebar();
        return;
      }

      closeSidebar();
      event.preventDefault();
      event.stopPropagation();
    };

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') closeSidebar();
    };

    document.addEventListener('click', handleDocumentClick, true);
    window.addEventListener('keydown', handleKeyDown);
    mediaQuery.addEventListener('change', closeSidebar);

    return () => {
      document.removeEventListener('click', handleDocumentClick, true);
      window.removeEventListener('keydown', handleKeyDown);
      mediaQuery.removeEventListener('change', closeSidebar);
    };
  }, []);

  return (
    <>
      <aside className="qb-release-banner" role="status" aria-live="polite">
        <strong>{copy.title}</strong>
        <span>{copy.detail}</span>
      </aside>
      {mobileSidebarOpen ? (
        <button
          type="button"
          className="qb-mobile-sidebar-mask"
          aria-label={copy.closeMenu}
        />
      ) : null}
    </>
  );
}
